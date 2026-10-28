"""A closed creator disagreement can yield one private replayable witness."""
from copy import deepcopy
from dataclasses import replace
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import (
    ETTODAY_CONFLICT_ORIGINAL_QUESTION, PendingOriginal,
    private_ettoday_conflict_original_permit, retain_original,
)
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday_attribution_predicates as prior
from test_news_ettoday_credit_declarations import queue, settings

old, literal = prior.old, prior.literal
URL, LAYOUT = prior.URL, prior.LAYOUT
ARTICLE_ID = URL.rsplit('/', 1)[-1]


def page():
    soup = literal.page()
    literal.native.update_schema(soup, creator=[ARTICLE_ID+'-另一姓名', ARTICLE_ID])
    return literal.native.BeautifulSoup(str(soup), 'lxml')


def response(soup=None, **changes):
    return old.response(page() if soup is None else soup, **changes)


def close63(queue, settings):
    prior.close62(queue, settings)
    request = prior.submit(queue, settings)
    http = old.http_stub(response())
    job = old.claim(queue, settings, http)
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    return request


def submit(queue, settings):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason='Private original for the already observed creator conflict', evidence_contract_version=68)


def expected(queue):
    return diagnostic._prior_ettoday_creator_conflict(queue.db, queue.registry.sources['ettoday'], URL)


def test_owner_retains_one_exact_private_response_and_preserves_completed_work(queue, settings):
    previous = close63(queue, settings)
    before = old.head(queue)
    request = submit(queue, settings)
    http = old.http_stub(response()); job = old.claim(queue, settings, http)
    pending = diagnostic.fetch_candidate(http, job)
    assert isinstance(pending, PendingOriginal)
    assert pending.permit['question'] == ETTODAY_CONFLICT_ORIGINAL_QUESTION
    assert literal.SIGNATURE not in json.dumps(pending.facts, ensure_ascii=False)
    assert literal.native.PROSE not in json.dumps(pending.facts, ensure_ascii=False)
    diagnostic.finish_candidate(queue, job, result=pending)
    status = diagnostic.candidate_status(queue.root, request['request_id'])
    assert status['state'] == 'observed' and status['raw_retention_state'] == 'retained_verified'
    reference = status['result_json']['private_original']
    original = pq.ParquetFile(queue.root/reference['path']).read().to_pylist()[0]
    assert original['raw_response_body'] == pending.response.body
    assert original['source_document_url'] == URL and original['transport_redirects_json'] == '[]'
    assert not original['accepted_as_article'] and not original['publish'] and not original['remote_training_export']
    assert (queue.root/reference['path']).stat().st_mode & 0o777 == 0o600
    assert old.head(queue) == before and http.calls == [URL]
    assert diagnostic.candidate_status(queue.root, previous['request_id'])['state'] == 'observed'
    assert not submit(queue, settings)['submitted']
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()


@pytest.mark.parametrize('mutation', ['missing', 'uncertain', 'source', 'head', 'invalid_json',
    'raw', 'creator', 'stage', 'attempt_backwards', 'clock_backwards'])
def test_submission_requires_unchanged_closed_conflict(queue, settings, mutation):
    if mutation != 'missing':
        request = close63(queue, settings)
        row = queue.db.execute('SELECT request_json,result_json FROM native_candidate_jobs WHERE request_id=?',
            (request['request_id'],)).fetchone()
        old_request, result = json.loads(row['request_json']), json.loads(row['result_json'])
        with queue.db:
            if mutation == 'uncertain':
                queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (request['request_id'],))
            elif mutation == 'source':
                old_request['source']['data_fingerprint'] = 'a'*64
                queue.db.execute('UPDATE native_candidate_jobs SET request_json=? WHERE request_id=?', (json.dumps(old_request), request['request_id']))
            elif mutation == 'head': queue.db.execute("UPDATE articles SET error_code='changed'")
            elif mutation == 'invalid_json':
                queue.db.execute("UPDATE native_candidate_jobs SET result_json='invalid' WHERE request_id=?", (request['request_id'],))
            elif mutation == 'attempt_backwards': queue.db.execute('UPDATE articles SET attempts=attempts-1')
            elif mutation == 'clock_backwards': queue.db.execute("UPDATE articles SET last_checked_at_utc='2025-01-01T00:00:00+00:00'")
            else:
                if mutation == 'raw': result['raw_response_retained'] = True
                elif mutation == 'creator': result['ettoday_attribution_predicates']['creator_values'][1] = '999'
                else: result['ettoday_attribution_predicates']['parser_outcomes'][1]['attribution_failure_stage'] = None
                queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?', (json.dumps(result), request['request_id']))
    with pytest.raises(ValueError): submit(queue, settings)


@pytest.mark.parametrize('mutation', ['canonical', 'schema', 'paid', 'hidden', 'clock', 'credit',
    'creator', 'foreign_publisher', 'foreign_author', 'iframe', 'reprint', 'duplicate_story'])
def test_response_identity_conflict_rights_and_geometry_must_still_match(queue, settings, mutation):
    close63(queue, settings); soup = page()
    if mutation == 'canonical': soup.select_one('link[rel=canonical]')['href'] = URL+'0'
    elif mutation == 'schema': literal.native.update_schema(soup, url=URL+'0')
    elif mutation == 'paid': literal.native.update_schema(soup, isAccessibleForFree=False)
    elif mutation == 'hidden': soup.select_one('.story')['hidden'] = ''
    elif mutation == 'clock': soup.select_one('meta[name=pubdate]')['content'] = literal.native.MOD
    elif mutation == 'credit': soup.select_one('#credit').string = '記埋不同作者／台北報導'
    elif mutation == 'creator': literal.native.update_schema(soup, creator=[ARTICLE_ID+'-不同聲明', ARTICLE_ID])
    elif mutation == 'foreign_publisher': literal.native.update_schema(soup, publisher={'@type':'Organization','name':'其他來源'})
    elif mutation == 'foreign_author': literal.native.update_schema(soup, author={'@type':'Person','name':'不同作者'})
    elif mutation == 'iframe': soup.select_one('.story').append(soup.new_tag('iframe'))
    elif mutation == 'reprint': soup.select_one('#prose').string = '本文轉載自未確認來源'
    else: soup.select_one('.subject_article').append(deepcopy(soup.select_one('.story')))
    with pytest.raises(NewsError):
        private_ettoday_conflict_original_permit(response(soup), queue.registry.sources['ettoday'], URL, expected(queue))


@pytest.mark.parametrize('changes', [dict(status=403), dict(status=429), dict(status=500), dict(body=b''),
    dict(body=b'x'*((2 << 20)+1)), dict(url=URL+'0'), dict(headers={'content-type':'application/json'})])
def test_transport_failures_never_grant_original(queue, settings, changes):
    close63(queue, settings)
    with pytest.raises(NewsError):
        private_ettoday_conflict_original_permit(response(**changes), queue.registry.sources['ettoday'], URL, expected(queue))


@pytest.mark.parametrize('change', ['head', 'prior'])
def test_claim_rejects_a_changed_head_or_predecessor_without_http(queue, settings, change):
    previous = close63(queue, settings); submit(queue, settings); diagnostic.pump_candidates(queue)
    with queue.db:
        if change == 'head': queue.db.execute('UPDATE articles SET attempts=attempts+1')
        else: queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (previous['request_id'],))
    http = old.http_stub(response())
    assert diagnostic.claim_candidate(queue, http, settings, 'ettoday') is None
    assert http.calls == []


@pytest.mark.parametrize('change', ['lease', 'due', 'body', 'head', 'prior'])
def test_claim_and_completion_recheck_head_and_predecessor(queue, settings, change):
    previous = close63(queue, settings); submit(queue, settings)
    http = old.http_stub(response()); job = old.claim(queue, settings, http)
    pending = diagnostic.fetch_candidate(http, job)
    with queue.db:
        if change == 'lease': queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=?", (time.time()+3600,))
        elif change == 'due': queue.db.execute('UPDATE articles SET due=?', (time.time()+3600,))
        elif change == 'body': queue.db.execute('UPDATE articles SET body_sha256=?', ('c'*64,))
        elif change == 'head': queue.db.execute('UPDATE articles SET attempts=attempts+1')
        else: queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (previous['request_id'],))
    diagnostic.finish_candidate(queue, job, result=pending)
    status = diagnostic.candidate_status(queue.root, job['request_id'])
    assert status['state'] == 'observed_error'
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*.parquet'))


def test_restarted_uncertain_observation_is_never_replayed(queue, settings):
    close63(queue, settings); request = submit(queue, settings)
    http = old.http_stub(response()); old.claim(queue, settings, http)
    queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (request['request_id'],))
    queue.db.commit(); queue.close()
    restarted = NewsQueue(queue.root, queue.registry)
    try:
        assert not submit(restarted, settings)['submitted']
        diagnostic.pump_candidates(restarted)
        assert diagnostic.claim_candidate(restarted, http, settings, 'ettoday') is None
        assert diagnostic.candidate_status(queue.root, request['request_id'])['state'] == 'uncertain_outcome'
        assert not http.calls
    finally: restarted.close()


def test_private_writer_checks_response_again_and_question_key_is_stable(queue, settings):
    close63(queue, settings); submit(queue, settings)
    http = old.http_stub(response()); job = old.claim(queue, settings, http)
    pending = diagnostic.fetch_candidate(http, job)
    changed = replace(pending.response, body=pending.response.body.replace(literal.SIGNATURE.encode(), b'unknown'))
    with pytest.raises((NewsError, ValueError)):
        retain_original(queue, job, PendingOriginal(changed, pending.facts, pending.permit))
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*.parquet'))
    source = queue.registry.sources['ettoday']
    assert candidate_evidence_allowed(source, source, 68)
    assert not candidate_evidence_allowed(replace(source, embedded_article_layout='ettoday_visible_article_v40'), source, 68)
    target = {'provider':'ettoday', 'id':'a'*64}
    assert diagnostic._key(target, 'b'*64, evidence_contract_version=68) == diagnostic._key(target, 'c'*64, evidence_contract_version=68)
    assert diagnostic._key(target, 'b'*64, evidence_contract_version=68) != diagnostic._key(target, 'b'*64, evidence_contract_version=63)
