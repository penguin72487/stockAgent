"""Finite current attribution witnesses never become body/author admission."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import ORIGINAL_EVIDENCE_VERSIONS
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_ettoday import ETTODAY_ATTRIBUTION_PREDICATES_QUESTION
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday_finance_literal_role_v42 as literal
import test_news_ettoday_prelude_excerpt as excerpt
from test_news_ettoday_credit_declarations import queue, settings

old=excerpt.previous
URL,LAYOUT=old.URL,old.LAYOUT


def response(soup=None,**changes):
    return old.response(literal.page() if soup is None else soup,**changes)


def facts(queue,envelope=None):
    source=queue.registry.sources['ettoday']
    return candidate_response_metadata(response() if envelope is None else envelope,source,source,
        evidence_contract_version=63,requested_url=URL)


def submit(queue,settings):
    return diagnostic.submit_candidate(queue.root,settings,queue.registry,URL,LAYOUT,
        reason='Distinct native author/creator and parser refusal facts after a closed prelude',evidence_contract_version=63)


def close62(queue,settings):
    excerpt.close61(queue,settings)
    request=excerpt.submit(queue,settings);http=old.http_stub(response())
    job=old.claim(queue,settings,http)
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
    diagnostic.pump_candidates(queue)
    return request


def test_exact_creator_predicates_and_refusal_sites_do_not_export_prose_or_grant_acceptance(queue):
    value=facts(queue);p=value['ettoday_attribution_predicates']
    assert p['creator_values']==['123-'+literal.NAME,'123'] and p['literal_role_name']==literal.NAME
    assert p['creator_identifier_valid'] and p['creator_matches_literal_role_name']
    assert p['native_authors']==[{'type':'Organization','name':'ETtoday新聞雲'}]
    assert p['prelude_text_sha256']==hashlib.sha256(literal.SIGNATURE.encode()).hexdigest()
    before,after=p['parser_outcomes']
    assert before['version']==41 and before['attribution_failure_stage']=='bare_reporter_creator_mismatch'
    assert before['error_code']=='article_attribution_not_verified' and before['failure_sites']
    assert after['version']==42 and after['parse_succeeded'] and after['error_code'] is None
    assert diagnostic._ettoday_attribution_predicates_result(value,URL)
    assert value['public_schema_declarations_retained'] and value['raw_or_names_or_body_retained']
    assert not value['accepted_as_article'] and not value['raw_or_body_retained']
    assert not p['author_or_supplier_accepted'] and not p['source_values_rewritten']
    rendered=json.dumps(value,ensure_ascii=False)
    assert literal.native.PROSE not in rendered and literal.SIGNATURE not in rendered and 'SECRET' not in rendered
    assert 'private_original' not in value and 'private_original_permit' not in value
    assert 63 not in ORIGINAL_EVIDENCE_VERSIONS


def test_foreign_creator_is_reported_literally_without_rewriting_the_signature_or_accepting_it(queue):
    soup=literal.page();literal.native.update_schema(soup,creator=['456-另一姓名','456'],
        author={'@type':'Person','name':'原始宣告','url':'https://example.org/SECRET'})
    p=facts(queue,response(soup))['ettoday_attribution_predicates']
    assert p['creator_values']==['456-另一姓名','456'] and not p['creator_matches_literal_role_name']
    assert p['native_authors']==[{'type':'Person','name':'原始宣告'}]
    assert p['parser_outcomes'][1]['attribution_failure_stage']=='bare_reporter_creator_mismatch'
    assert not p['parser_outcomes'][1]['parse_succeeded'] and 'SECRET' not in json.dumps(p)


@pytest.mark.parametrize('creator',[None,{'secret':'discard'},['x'*161,'1'],['1',1],['x']*5])
def test_unbounded_or_nonstring_creator_values_are_withheld(queue,creator):
    soup=literal.page();literal.native.update_schema(soup,creator=creator)
    p=facts(queue,response(soup))['ettoday_attribution_predicates']
    assert p['creator_values'] is None and p['creator_values_withheld'] is (creator is not None)
    assert not p['creator_matches_literal_role_name']
    assert diagnostic._ettoday_attribution_predicates_result(facts(queue,response(soup)),URL)


@pytest.mark.parametrize('mutation',['canonical','schema','paid','hidden','clock'])
def test_native_document_identity_public_access_and_clocks_remain_required(queue,mutation):
    soup=literal.page()
    if mutation=='canonical':soup.select_one('link[rel=canonical]')['href']=URL+'0'
    elif mutation=='schema':literal.native.update_schema(soup,url=URL+'0')
    elif mutation=='paid':literal.native.update_schema(soup,isAccessibleForFree=False)
    elif mutation=='hidden':soup.select_one('.story')['hidden']=''
    else:soup.select_one('meta[name=pubdate]')['content']=literal.native.MOD
    with pytest.raises(NewsError):facts(queue,response(soup))


@pytest.mark.parametrize('change',[dict(status=403),dict(status=429),dict(status=500),dict(body=b''),
    dict(body=b'x'*((2<<20)+1)),dict(url=URL+'0'),dict(headers={'content-type':'application/json'})])
def test_refused_or_nonliteral_transport_does_not_release_names(queue,change):
    with pytest.raises(NewsError):facts(queue,response(**change))


@pytest.mark.parametrize('mutation',['missing','uncertain','source','head','hash','raw','invalid_json','attempt_backwards','clock_backwards'])
def test_question_requires_closed_valid_prelude_with_unchanged_identity(queue,settings,mutation):
    if mutation!='missing':
        req=close62(queue,settings)
        row=queue.db.execute('SELECT request_json,result_json FROM native_candidate_jobs WHERE request_id=?',(req['request_id'],)).fetchone()
        request,result=json.loads(row['request_json']),json.loads(row['result_json'])
        with queue.db:
            if mutation=='uncertain':queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(req['request_id'],))
            elif mutation=='source':
                request['source']['data_fingerprint']='a'*64
                queue.db.execute('UPDATE native_candidate_jobs SET request_json=? WHERE request_id=?',(json.dumps(request),req['request_id']))
            elif mutation=='head':queue.db.execute("UPDATE articles SET error_code='changed'")
            elif mutation=='invalid_json':queue.db.execute("UPDATE native_candidate_jobs SET result_json='invalid' WHERE request_id=?",(req['request_id'],))
            elif mutation=='attempt_backwards':queue.db.execute('UPDATE articles SET attempts=attempts-1')
            elif mutation=='clock_backwards':queue.db.execute("UPDATE articles SET last_checked_at_utc='2025-01-01T00:00:00+00:00'")
            else:
                if mutation=='hash':result['ettoday_unclassified_prelude']['text_sha256']='a'*64
                else:result['raw_response_retained']=True
                queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',(json.dumps(result),req['request_id']))
    with pytest.raises(ValueError):submit(queue,settings)


def test_monotonic_failed_attempt_reuses_old_excerpt_without_reopening_it(queue,settings):
    oldreq=close62(queue,settings)
    with queue.db:
        queue.db.execute("UPDATE articles SET attempts=attempts+1,last_checked_at_utc='2026-02-01T00:00:00+00:00'")
    before=old.head(queue);budgets=list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))
    req=submit(queue,settings);http=old.http_stub(response());job=old.claim(queue,settings,http)
    assert job['evidence_contract_version']==63 and req['request_id']!=oldreq['request_id']
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job));diagnostic.pump_candidates(queue)
    value=diagnostic.candidate_status(queue.root,req['request_id'])
    assert value['state']=='observed' and value['public_schema_declarations_retained'] and value['raw_or_names_or_body_retained']
    assert not value['raw_or_body_retained'] and not value['raw_response_retained'] and not value['full_body_retained']
    assert value['result_json']['ettoday_attribution_predicates']['parser_outcomes'][1]['parse_succeeded']
    assert old.head(queue)==before and http.calls==[URL]
    assert list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))==budgets
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone() and not submit(queue,settings)['submitted']
    archived=json.loads((queue.root/'reviewed_repairs/candidate_diagnostics/processed'/(req['request_id']+'.outcome.json')).read_bytes())
    assert archived['public_schema_declarations_retained'] and not archived['raw_or_body_retained']
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))


@pytest.mark.parametrize('mutation',['lease','due','body','head','prior62'])
def test_current_dispatch_still_rechecks_head_lease_due_and_prior_evidence(queue,settings,mutation):
    req=close62(queue,settings);submit(queue,settings);diagnostic.pump_candidates(queue)
    with queue.db:
        if mutation=='lease':queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=?",(time.time()+600,))
        elif mutation=='due':queue.db.execute('UPDATE articles SET due=?',(time.time()+600,))
        elif mutation=='body':queue.db.execute('UPDATE articles SET body_sha256=?',('a'*64,))
        elif mutation=='head':queue.db.execute('UPDATE articles SET attempts=attempts+1')
        else:queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(req['request_id'],))
    http=old.http_stub(response())
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None and not http.calls


def test_uncertain_reservation_reports_name_retention_unknown_and_is_not_replayed(queue,settings):
    close62(queue,settings);req=submit(queue,settings);http=old.http_stub(response())
    old.claim(queue,settings,http);queue.close();owner=NewsQueue(queue.root,queue.registry)
    try:
        diagnostic.pump_candidates(owner);value=diagnostic.candidate_status(owner.root,req['request_id'])
        assert value['state']=='uncertain_outcome' and value['public_schema_declarations_retained'] is None
        assert value['raw_or_names_or_body_retained'] is None and not value['raw_or_body_retained']
        assert not submit(owner,settings)['submitted']
    finally:owner.close()


@pytest.mark.parametrize('mutation',['raw','body','author','accept','identity','question','extra','extra_nested','long_creator','long_author','site','outcomes'])
def test_completion_rejects_spilled_content_or_false_acceptance_flags(queue,settings,mutation):
    close62(queue,settings);submit(queue,settings);http=old.http_stub(response());job=old.claim(queue,settings,http)
    result=diagnostic.fetch_candidate(http,job);p=result['ettoday_attribution_predicates']
    if mutation=='raw':result['raw_response_retained']=True
    elif mutation=='body':result['full_body_retained']=True
    elif mutation=='author':p['author_or_supplier_accepted']=True
    elif mutation=='accept':result['accepted_as_article']=True
    elif mutation=='identity':result['identity_or_alias_accepted']=True
    elif mutation=='question':p['question']='wrong'
    elif mutation=='extra':result['body']='not permitted'
    elif mutation=='extra_nested':p['raw_schema']={'not':'permitted'}
    elif mutation=='long_creator':p['creator_values']=['x'*161]
    elif mutation=='long_author':p['native_authors'][0]['name']='x'*81
    elif mutation=='site':p['parser_outcomes'][0]['failure_sites'][0]['secret']='not permitted'
    else:p['parser_outcomes'].append(deepcopy(p['parser_outcomes'][0]))
    diagnostic.finish_candidate(queue,job,result=result)
    value=diagnostic.candidate_status(queue.root,job['request_id'])
    assert value['state']=='observed_error' and not value['public_schema_declarations_retained']


def test_question_key_cannot_be_rearmed_by_changing_source_fingerprint(queue):
    source=queue.registry.sources['ettoday'];target={'provider':'ettoday','id':'a'*64}
    assert candidate_evidence_allowed(source,source,63)
    oldsource=replace(source,embedded_article_layout='ettoday_visible_article_v40')
    assert not candidate_evidence_allowed(oldsource,oldsource,63)
    assert diagnostic._key(target,'b'*64,evidence_contract_version=63)==diagnostic._key(target,'c'*64,evidence_contract_version=63)
    assert diagnostic._key(target,'b'*64,evidence_contract_version=63)!=diagnostic._key(target,'b'*64,evidence_contract_version=62)
