"""A first-paragraph signature can coexist with prose, without article admission."""
from dataclasses import replace
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal
from downloader.news_fulltext_candidates import candidate_response_metadata
from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_PREFIX_QUESTION
from downloader.news_fulltext_policy import NewsError, extract_article
from test_news_contentplatform import URL, RAW
from test_news_contentplatform_originals import owner, head, submit, response, http_stub
from test_news_contentplatform_roots import nested_page, root_submit
from test_news_fulltext import config


def prefix_page(inner=None):
    return nested_page(inner=inner if inner is not None else
        '<img src="/never.jpg"><p>【許家源記者 / 綜合報導】 '+RAW+'</p>')


def prefix_submit(queue, settings, reason='First written signed prefix, preserving closed whole-credit questions'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL,
        queue.registry.sources['contentplatform'].embedded_article_layout,
        reason=reason, evidence_contract_version=53)


def close_whole_credit_refusals(queue, settings):
    identifiers = []
    for create in (submit, root_submit):
        requested = create(queue, settings)
        diagnostic.pump_candidates(queue)
        http = http_stub(response(body=prefix_page()))
        job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
        assert job is not None
        result = diagnostic.fetch_candidate(http, job)
        assert not isinstance(result, PendingOriginal)
        diagnostic.finish_candidate(queue, job, result=result)
        identifiers.append(requested['request_id'])
    return identifiers


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('leading', ['', '<img src="/never.jpg">',
    '<div><figure><img src="/never.jpg"></figure></div>'])
def test_prefix_after_textless_media_keeps_one_private_original_without_admitting_body(owner, parser, leading):
    queue, settings = owner
    source = replace(queue.registry.sources['contentplatform'], html_parser=parser)
    raw = prefix_page(leading+'<p>【許家源記者 / 綜合報導】 '+RAW+'</p>')
    facts = candidate_response_metadata(response(body=raw), source, source,
        evidence_contract_version=53, requested_url=URL)
    permit = facts['private_original_permit']
    shape = facts['native_prefix_credit_shape']
    assert permit['question'] == CONTENTPLATFORM_PREFIX_QUESTION
    assert shape['signed_prefix_matches_declared_author'] and shape['signed_prefix_followed_by_prose']
    assert not shape['whole_paragraph_credit_matches_declared_author']
    assert not shape['article_accepted'] and not shape['body_geometry_accepted']
    assert not shape['source_text_exported'] and RAW not in json.dumps(shape, ensure_ascii=False)
    with pytest.raises(NewsError):
        extract_article(raw, source, page_url=URL)


@pytest.mark.parametrize('inner', [
    '<p>他人報導在前</p><p>【許家源記者 / 綜合報導】 '+RAW+'</p>',
    '文字在前<p>【許家源記者 / 綜合報導】 '+RAW+'</p>',
    '<p>【別人記者 / 綜合報導】 '+RAW+'</p>',
    '<p>前文【許家源記者 / 綜合報導】 '+RAW+'</p>',
    '<p hidden>【許家源記者 / 綜合報導】 '+RAW+'</p>',
    '<figure><img src="/never.jpg"><figcaption>先前文字</figcaption></figure>'
        '<p>【許家源記者 / 綜合報導】 '+RAW+'</p>',
    '<figure><a href="/never"><img src="/never.jpg"></a></figure>'
        '<p>【許家源記者 / 綜合報導】 '+RAW+'</p>',
    '<secret-source-words>文字</secret-source-words><p>【許家源記者 / 綜合報導】 '+RAW+'</p>',
    '<img src="/never.jpg">'*5+'<p>【許家源記者 / 綜合報導】 '+RAW+'</p>',
    '<p>【許家源記者 / 綜合報導】 '+RAW+'</p><p hidden>未顯示</p>',
])
def test_never_skips_prior_words_names_visibility_or_unknown_media(owner, inner):
    queue, _ = owner
    source = queue.registry.sources['contentplatform']
    facts = candidate_response_metadata(response(body=prefix_page(inner)), source, source,
        evidence_contract_version=53, requested_url=URL)
    assert 'private_original_permit' not in facts
    exported = json.dumps(facts, ensure_ascii=False)
    assert all(word not in exported for word in (RAW, '許家源', '他人報導', 'secret-source-words'))


def test_new_relation_survives_restart_and_leaves_closed_jobs_heads_and_daily_charges_intact(owner, monkeypatch):
    from downloader.news_fulltext_queue import NewsQueue
    queue, settings = owner
    old_head = head(queue)
    charges = list(map(tuple, queue.db.execute('SELECT * FROM queue_settings')))
    close_whole_credit_refusals(queue, settings)
    closed = list(map(tuple, queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('No second HTTP owner'))
    requested = prefix_submit(queue, settings)
    assert prefix_submit(queue, settings, reason='Changed wording')['request_id'] == requested['request_id']
    diagnostic.pump_candidates(queue)
    http = http_stub(response(body=prefix_page()))
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, PendingOriginal) and http.calls == [URL]
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, requested['request_id'])
    ref = status['result_json']['private_original']
    record, = pq.ParquetFile(queue.root/ref['path']).read().to_pylist()
    assert record['raw_response_body'] == prefix_page()
    assert not record['accepted_as_article'] and not record['historical_point_in_time']
    assert head(queue) == old_head
    assert list(map(tuple, queue.db.execute('SELECT * FROM queue_settings'))) == charges
    assert list(map(tuple, queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id<>? ORDER BY request_id',
        (requested['request_id'],)))) == closed
    queue.close()
    restarted = NewsQueue(queue.root, queue.registry)
    try:
        assert not prefix_submit(restarted, settings)['submitted']
        assert not submit(restarted, settings)['submitted'] and not root_submit(restarted, settings)['submitted']
    finally:
        restarted.close()


@pytest.mark.parametrize('change', ['retained', 'uncertain', 'future', 'source', 'head'])
def test_predecessor_is_revalidated_at_reservation_without_a_request(owner, change):
    queue, settings = owner
    _, prior = close_whole_credit_refusals(queue, settings)
    prefix_submit(queue, settings)
    with queue.db:
        row = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (prior,)).fetchone()
        result = json.loads(row['result_json'])
        if change == 'retained':
            result['private_original'] = {'path': 'already-retained'}
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?', (json.dumps(result),prior))
        elif change == 'uncertain':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (prior,))
        elif change == 'future':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?", (prior,))
        elif change == 'source':
            request = json.loads(row['request_json']);request['source']['authorization_fingerprint']='0'*64
            queue.db.execute('UPDATE native_candidate_jobs SET request_json=? WHERE request_id=?', (json.dumps(request),prior))
        else:
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
    diagnostic.pump_candidates(queue)
    http = http_stub(response(body=prefix_page()))
    assert diagnostic.claim_candidate(queue, http, settings, 'contentplatform') is None
    assert http.calls == []


@pytest.mark.parametrize('gate', ['deadline', 'gateway', 'robots'])
def test_original_source_gates_still_defer_the_new_question(owner, gate):
    queue, settings = owner
    close_whole_credit_refusals(queue, settings)
    prefix_submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(response(body=prefix_page()))
    if gate == 'deadline': http.provider_not_before['contentplatform'] = time.time()+3600
    elif gate == 'gateway': http.blocked_gateway_hosts = lambda _: ('www.contentplatform.info',)
    else: http.blocked_robots_origins = lambda _: ('https://www.contentplatform.info',)
    assert diagnostic.claim_candidate(queue, http, settings, 'contentplatform') is None
    assert http.calls == []
