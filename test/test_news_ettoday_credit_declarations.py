"""Finite credit evidence cannot admit an author, article, or second request."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import ORIGINAL_EVIDENCE_VERSIONS
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata, native_candidate_source
from downloader.news_fulltext_ettoday import ETTODAY_CREDIT_DECLARATION_QUESTION, ettoday_finance_credit_declarations
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday as native

URL, LAYOUT = native.URLS[0], 'ettoday_visible_article_v41'
CREDIT = '文／合成研究提供'


def page():
    soup = native.page('finance')
    soup.select_one('#credit').string = CREDIT
    return soup


def response(soup=None, **changes):
    return replace(Response(URL, 200, {'content-type': 'text/html; charset=utf-8', 'set-cookie': 'SECRET'},
        str(page() if soup is None else soup).encode()), **changes)


@pytest.fixture
def settings(tmp_path):
    entry = native.recipe(); entry['embedded_article_layout'] = LAYOUT
    return dict(contract_version=1, root=str(tmp_path), collection_purpose='personal_research',
        publish=False, remote_training_export=False, providers=[entry], min_free_disk_bytes=1,
        workers=2, exclusive_http_owner=False, adaptive_rate={'enabled': False})


@pytest.fixture
def queue(settings, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    queue.add_articles([dict(url=URL)])
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=?',
            ('article_attribution_not_verified', 'article_attribution_not_verified', time.time()-60,
                '2026-01-01T00:00:00+00:00'))
    yield queue
    queue.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def submit(queue, settings, **changes):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, changes.pop('url', URL), LAYOUT,
        reason=changes.pop('reason', 'Unasked short native source-credit declaration'),
        evidence_contract_version=61, **changes)


def http_stub(envelope=None):
    calls = []
    def get(url, *, follow_redirects):
        assert follow_redirects is False
        calls.append(url)
        return response() if envelope is None else envelope
    return SimpleNamespace(calls=calls, get=get, provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


def claim(queue, settings, http):
    deadline=time.monotonic()+3
    while time.monotonic()<deadline:
        diagnostic.pump_candidates(queue)
        job=diagnostic.claim_candidate(queue,http,settings,'ettoday')
        if job is not None:
            return job
        time.sleep(.005)
    pytest.fail('The original immutable diagnostic did not become ready')


def test_header_is_literal_bounded_and_does_not_accept_an_article(queue, monkeypatch):
    source=queue.registry.sources['ettoday']
    with pytest.raises(NewsError,match='article_(?:attribution_not_verified|rights_or_scope_excluded)'):
        extract_article(response().body,source,page_url=URL)
    monkeypatch.setattr('downloader.news_fulltext_ettoday.ettoday_visible_article',
        lambda *a,**k:pytest.fail('A header question cannot accept a body'))
    facts=candidate_response_metadata(response(),source,source,evidence_contract_version=61,requested_url=URL)
    assert facts['question']==ETTODAY_CREDIT_DECLARATION_QUESTION
    header=facts['ettoday_native_header']; declaration,=header['native_credit_declarations']
    assert declaration['text']==CREDIT and declaration['text_sha256']==hashlib.sha256(CREDIT.encode()).hexdigest()
    assert not declaration['attribution_accepted'] and not header['author_or_supplier_accepted']
    assert header['published_at_raw']==native.PUB and header['modified_at_raw']==native.MOD
    assert not facts['accepted_as_article'] and not facts['raw_or_body_retained']
    assert facts['raw_or_names_or_body_retained'] and facts['public_header_declarations_retained']
    assert native.PROSE not in json.dumps(facts,ensure_ascii=False) and 'SECRET' not in json.dumps(facts)
    assert 'private_original_permit' not in facts and 61 not in ORIGINAL_EVIDENCE_VERSIONS


@pytest.mark.parametrize('mutation',['hidden','br_prose','nested','link','caption','arbitrary_prose','signature_prose','oversize','ninth'])
def test_only_short_visible_prefixed_credit_headers_are_exported(queue,mutation):
    soup=page(); credit=soup.select_one('#credit')
    if mutation=='hidden':credit['style']='display:none'
    elif mutation=='br_prose':
        credit.append(soup.new_tag('br'));credit.append(native.PROSE)
    elif mutation=='nested':credit.string.wrap(soup.new_tag('span'))
    elif mutation=='link':
        link=soup.new_tag('a',href='https://example.org/SECRET');link.string=CREDIT;credit.clear();credit.append(link)
    elif mutation=='caption':credit.string='▲'+CREDIT
    elif mutation=='arbitrary_prose':credit.string='此為一般短正文，無署名或來源前綴。'
    elif mutation=='signature_prose':credit.string='記者合成記者／台北報導。這句是正文，不能當成署名匯出。'
    elif mutation=='oversize':credit.string='文／'+'字'*161
    else:
        credit.extract()
        for _ in range(8):
            p=soup.new_tag('p');p.string='一般正文';soup.select_one('.story').append(p)
        soup.select_one('.story').append(credit)
    facts=candidate_response_metadata(response(soup),queue.registry.sources['ettoday'],queue.registry.sources['ettoday'],
        evidence_contract_version=61,requested_url=URL)
    assert not facts['ettoday_native_header']['native_credit_declarations']
    assert not facts['raw_or_names_or_body_retained']


@pytest.mark.parametrize('mutation',['canonical','og_url','headline','schema_identity','extra_schema','paid','clock','duplicate_story','hidden_body'])
def test_foreign_conflicting_paid_or_hidden_documents_do_not_release_headers(queue,mutation):
    soup=page()
    if mutation=='canonical':soup.select_one('link[rel=canonical]')['href']=URL+'0'
    elif mutation=='og_url':soup.select_one('meta[property="og:url"]')['content']=URL+'0'
    elif mutation=='headline':soup.select_one('h1')['hidden']=''
    elif mutation=='schema_identity':native.update_schema(soup,url=URL+'0')
    elif mutation=='extra_schema':
        soup.head.append(deepcopy(soup.select_one('script#report')))
    elif mutation=='paid':native.update_schema(soup,isAccessibleForFree=False)
    elif mutation=='clock':soup.select_one('meta[name=pubdate]')['content']=native.MOD
    elif mutation=='duplicate_story':soup.select_one('.subject_article').append(deepcopy(soup.select_one('.story')))
    else:soup.select_one('.story')['hidden']=''
    with pytest.raises(NewsError):
        ettoday_finance_credit_declarations(response(soup),queue.registry.sources['ettoday'],URL)


@pytest.mark.parametrize('changes',[
    {'status':403},{'status':429},{'status':500},{'body':b''},{'body':b'x'*((2<<20)+1)},
    {'url':URL+'0'},{'headers':{'content-type':'application/json'}},
])
def test_transport_and_payload_guards_preserve_the_question_scope(queue,changes):
    with pytest.raises(NewsError):
        ettoday_finance_credit_declarations(response(**changes),queue.registry.sources['ettoday'],URL)


def test_old_questions_sources_and_routes_cannot_be_rearmed(queue,settings):
    source=queue.registry.sources['ettoday']
    assert native_candidate_source(settings,queue.registry,queue.registry.base,LAYOUT)==source
    for version in (1,27,28,29,30,31,32,33,51,52,60):
        assert not candidate_evidence_allowed(source,source,version)
    for changes in (dict(embedded_article_layout='ettoday_visible_article_v40'),dict(known_url_only=False),
            dict(allowed=False),dict(domains=('example.org',)),dict(selectors=('body',))):
        other=replace(source,**changes)
        assert not candidate_evidence_allowed(other,other,61)
    assert not candidate_evidence_allowed(source,replace(source,data_fingerprint='a'*64),61)
    for url in (native.URLS[1],URL+'?x=1',URL+'#credit',URL.replace('https:','http:')):
        with pytest.raises(NewsError):
            ettoday_finance_credit_declarations(response(url=url),source,url)


def test_single_owner_reserves_once_persists_only_headers_and_keeps_head(queue,settings):
    before=head(queue);budgets=list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))
    request=submit(queue,settings);http=http_stub();job=claim(queue,settings,http)
    assert queue.db.execute('SELECT state FROM native_candidate_jobs').fetchone()[0]=='reserved'
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
    status=diagnostic.candidate_status(queue.root,request['request_id'])
    assert status['state']=='observed' and http.calls==[URL]
    assert status['raw_or_names_or_body_retained'] and not status['raw_or_body_retained']
    assert status['result_json']['ettoday_native_header']['native_credit_declarations'][0]['text']==CREDIT
    assert head(queue)==before and list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))==budgets
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    assert not submit(queue,settings,reason='A new reason must reuse the closed handle')['submitted']
    target=json.loads(queue.db.execute('SELECT request_json FROM native_candidate_jobs').fetchone()[0])['target']
    assert diagnostic._key(target,'a'*64,evidence_contract_version=61)==diagnostic._key(target,'b'*64,evidence_contract_version=61)


@pytest.mark.parametrize('change',['lease','due','body','head'])
def test_current_head_protection_is_rechecked_before_claim(queue,settings,change):
    submit(queue,settings);diagnostic.pump_candidates(queue)
    with queue.db:
        if change=='lease':queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=?",(time.time()+3600,))
        elif change=='due':queue.db.execute('UPDATE articles SET due=?',(time.time()+3600,))
        elif change=='body':queue.db.execute('UPDATE articles SET body_sha256=?',('c'*64,))
        else:queue.db.execute('UPDATE articles SET attempts=attempts+1')
    http=http_stub()
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None and not http.calls


@pytest.mark.parametrize('blocked',['deadline','gateway','robots'])
def test_http_owner_cooldowns_and_refusals_are_never_reset(queue,settings,blocked):
    submit(queue,settings);diagnostic.pump_candidates(queue);http=http_stub()
    if blocked=='deadline':http.provider_not_before['ettoday']=time.time()+600
    elif blocked=='gateway':http.blocked_gateway_hosts=lambda _:('finance.ettoday.net',)
    else:http.blocked_robots_origins=lambda _:('https://finance.ettoday.net',)
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None and not http.calls


def test_uncertain_question_survives_owner_recovery_without_retry(queue,settings):
    request=submit(queue,settings);http=http_stub();claim(queue,settings,http)
    queue.close();restarted=NewsQueue(queue.root,queue.registry)
    try:
        diagnostic.pump_candidates(restarted)
        status=diagnostic.candidate_status(restarted.root,request['request_id'])
        assert status['state']=='uncertain_outcome' and status['raw_or_names_or_body_retained'] is None
        assert not status['raw_or_body_retained']
        assert not submit(restarted,settings)['submitted']
        assert diagnostic.claim_candidate(restarted,http,settings,'ettoday') is None and not http.calls
    finally:
        restarted.close()
