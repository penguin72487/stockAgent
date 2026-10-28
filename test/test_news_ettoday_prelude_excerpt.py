"""An explicitly unclassified excerpt is private evidence, never attribution."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import candidate_response_metadata, candidate_evidence_allowed
from downloader.news_fulltext_ettoday import ETTODAY_PRELUDE_EXCERPT_QUESTION
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday_credit_declarations as previous
from test_news_ettoday_credit_declarations import queue, settings

URL,LAYOUT=previous.URL,previous.LAYOUT
TEXT='合成未分類原生前文'


def page():
    soup=previous.page();soup.select_one('#credit').string=TEXT
    return soup


def response(soup=None,**changes):
    return previous.response(page() if soup is None else soup,**changes)


def submit(queue,settings):
    return diagnostic.submit_candidate(queue.root,settings,queue.registry,URL,LAYOUT,
        reason='Distinct unclassified short prelude after a closed empty credit question',evidence_contract_version=62)


def close61(queue,settings):
    request=previous.submit(queue,settings);http=previous.http_stub(response())
    job=previous.claim(queue,settings,http)
    facts=diagnostic.fetch_candidate(http,job)
    assert facts['ettoday_native_header']['native_credit_declarations']==[]
    diagnostic.finish_candidate(queue,job,result=facts);diagnostic.pump_candidates(queue)
    return request


def facts(queue,envelope=None):
    source=queue.registry.sources['ettoday']
    return candidate_response_metadata(response() if envelope is None else envelope,source,source,
        evidence_contract_version=62,requested_url=URL)


def test_excerpt_has_truthful_prose_flags_without_full_body_or_source_attributes(queue):
    soup=page();credit=soup.select_one('#credit');link=soup.new_tag('a',href='https://example.org/SECRET')
    link.string=TEXT;credit.clear();credit.append(link)
    value=facts(queue,response(soup));excerpt=value['ettoday_unclassified_prelude']
    assert value['question']==ETTODAY_PRELUDE_EXCERPT_QUESTION
    assert excerpt['text']==TEXT and excerpt['text_sha256']==hashlib.sha256(TEXT.encode()).hexdigest()
    assert excerpt['possibly_prose'] and excerpt['descendant_tags']==['a']
    assert value['raw_or_body_retained'] and value['raw_or_names_or_body_retained'] and value['diagnostic_excerpt_retained']
    assert not value['accepted_as_article'] and not value['raw_response_retained'] and not value['full_body_retained']
    assert not excerpt['attribution_accepted'] and not excerpt['author_or_supplier_accepted']
    rendered=json.dumps(value,ensure_ascii=False)
    assert 'SECRET' not in rendered and previous.native.PROSE not in rendered and '<a' not in rendered
    assert 'private_original' not in value and 'private_original_permit' not in value


@pytest.mark.parametrize('mutation',['image','caption','empty','long','script','frame','hidden','hidden_image',
    'hidden_caption','hidden_following','no_following','no_paragraph','wrong_order','canonical','schema','paid','clock'])
def test_public_identity_geometry_visibility_and_excerpt_bounds_are_required(queue,mutation):
    soup=page();image,caption,prelude,following=soup.select('.story > p')[:4]
    if mutation=='image':image.img.decompose()
    elif mutation=='caption':caption.string='一般段落不能當作圖說'
    elif mutation=='empty':prelude.clear()
    elif mutation=='long':prelude.string='字'*161
    elif mutation=='script':prelude.append(soup.new_tag('script'))
    elif mutation=='frame':prelude.append(soup.new_tag('iframe'))
    elif mutation=='hidden':prelude['hidden']=''
    elif mutation=='hidden_image':image['style']='display:none'
    elif mutation=='hidden_caption':caption['hidden']=''
    elif mutation=='hidden_following':following['hidden']=''
    elif mutation=='no_following':
        for n in list(prelude.find_next_siblings()):n.decompose()
    elif mutation=='no_paragraph':prelude.name='div'
    elif mutation=='wrong_order':image.insert_before(deepcopy(prelude))
    elif mutation=='canonical':soup.select_one('link[rel=canonical]')['href']=URL+'0'
    elif mutation=='schema':previous.native.update_schema(soup,url=URL+'0')
    elif mutation=='paid':previous.native.update_schema(soup,isAccessibleForFree=False)
    else:soup.select_one('meta[name=pubdate]')['content']=previous.native.MOD
    with pytest.raises(NewsError):facts(queue,response(soup))


@pytest.mark.parametrize('change',[dict(status=403),dict(status=429),dict(status=500),dict(body=b''),
    dict(body=b'x'*((2<<20)+1)),dict(url=URL+'0'),dict(headers={'content-type':'application/json'})])
def test_refused_or_nonliteral_transports_do_not_release_excerpts(queue,change):
    with pytest.raises(NewsError):facts(queue,response(**change))


@pytest.mark.parametrize('change',['missing','uncertain','nonempty','raw','source','head','invalid_json'])
def test_submission_requires_the_exact_closed_empty_question61(queue,settings,change):
    if change!='missing':
        req=close61(queue,settings)
        row=queue.db.execute('SELECT request_json,result_json FROM native_candidate_jobs WHERE request_id=?',(req['request_id'],)).fetchone()
        result=json.loads(row['result_json'])
        with queue.db:
            if change=='uncertain':queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome'")
            elif change=='head':queue.db.execute('UPDATE articles SET attempts=attempts+1')
            elif change=='invalid_json':queue.db.execute("UPDATE native_candidate_jobs SET result_json='bad'")
            elif change=='source':
                request=json.loads(row['request_json']);request['source']['data_fingerprint']='a'*64
                queue.db.execute('UPDATE native_candidate_jobs SET request_json=?',(json.dumps(request),))
            else:
                if change=='nonempty':result['ettoday_native_header']['native_credit_declarations']=[{'text':'合成聲明'}]
                else:result['raw_or_body_retained']=True
                queue.db.execute('UPDATE native_candidate_jobs SET result_json=?',(json.dumps(result),))
    with pytest.raises(ValueError):submit(queue,settings)


def test_resident_owner_retains_once_archives_honestly_and_never_accepts_body(queue,settings):
    old=close61(queue,settings);before=previous.head(queue)
    budgets=list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))
    req=submit(queue,settings);http=previous.http_stub(response());job=previous.claim(queue,settings,http)
    assert job['evidence_contract_version']==62 and req['request_id']!=old['request_id']
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job));diagnostic.pump_candidates(queue)
    result=diagnostic.candidate_status(queue.root,req['request_id'])
    assert result['state']=='observed' and result['diagnostic_excerpt_retained'] and result['raw_or_body_retained']
    assert not result['raw_response_retained'] and not result['full_body_retained']
    assert result['result_json']['ettoday_unclassified_prelude']['text']==TEXT and http.calls==[URL]
    assert previous.head(queue)==before and list(map(tuple,queue.db.execute('SELECT * FROM queue_settings')))==budgets
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    assert not submit(queue,settings)['submitted'] and not previous.submit(queue,settings)['submitted']
    archived=json.loads((queue.root/'reviewed_repairs/candidate_diagnostics/processed'/(req['request_id']+'.outcome.json')).read_bytes())
    assert archived['diagnostic_excerpt_retained'] and archived['raw_or_body_retained'] and not archived['raw_response_retained']
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))


@pytest.mark.parametrize('change',['lease','due','body','head','prior61'])
def test_current_guards_are_rechecked_at_dispatch(queue,settings,change):
    close61(queue,settings);submit(queue,settings);diagnostic.pump_candidates(queue)
    with queue.db:
        if change=='lease':queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=?",(time.time()+600,))
        elif change=='due':queue.db.execute('UPDATE articles SET due=?',(time.time()+600,))
        elif change=='body':queue.db.execute('UPDATE articles SET body_sha256=?',('a'*64,))
        elif change=='head':queue.db.execute('UPDATE articles SET attempts=attempts+1')
        else:queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE json_extract(request_json,'$.evidence_contract_version')=61")
    http=previous.http_stub(response())
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None and not http.calls


def test_uncertain_excerpt_reservation_is_not_retried_after_recovery(queue,settings):
    close61(queue,settings);req=submit(queue,settings);http=previous.http_stub(response())
    previous.claim(queue,settings,http);queue.close();owner=NewsQueue(queue.root,queue.registry)
    try:
        diagnostic.pump_candidates(owner);status=diagnostic.candidate_status(owner.root,req['request_id'])
        assert status['state']=='uncertain_outcome' and status['diagnostic_excerpt_retained'] is None
        assert status['raw_or_body_retained'] is None and not status['raw_response_retained']
        assert not submit(owner,settings)['submitted']
        assert diagnostic.claim_candidate(owner,http,settings,'ettoday') is None and not http.calls
    finally:owner.close()


@pytest.mark.parametrize('mutation',['oversize','author','body','identity','question'])
def test_writer_rejects_a_forged_excerpt_or_acceptance_claim(queue,settings,mutation):
    close61(queue,settings);req=submit(queue,settings);http=previous.http_stub(response())
    job=previous.claim(queue,settings,http);result=diagnostic.fetch_candidate(http,job)
    if mutation=='oversize':result['ettoday_unclassified_prelude']['text']='字'*161
    elif mutation=='author':result['ettoday_unclassified_prelude']['author_or_supplier_accepted']=True
    elif mutation=='body':result['full_body_retained']=True
    elif mutation=='identity':result['identity_or_alias_accepted']=True
    else:result['question']='different-question'
    diagnostic.finish_candidate(queue,job,result=result)
    status=diagnostic.candidate_status(queue.root,req['request_id'])
    assert status['state']=='observed_error' and not status['diagnostic_excerpt_retained']


def test_question62_does_not_unlock_old_sources_or_repeat_keys(queue):
    source=queue.registry.sources['ettoday']
    assert candidate_evidence_allowed(source,source,62)
    old=replace(source,embedded_article_layout='ettoday_visible_article_v40')
    assert not candidate_evidence_allowed(old,old,62)
    target={'provider':'ettoday','id':'a'*64}
    assert diagnostic._key(target,'b'*64,evidence_contract_version=62)==diagnostic._key(target,'c'*64,evidence_contract_version=62)
    assert diagnostic._key(target,'b'*64,evidence_contract_version=62)!=diagnostic._key(target,'b'*64,evidence_contract_version=61)
